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
import alluxio.client.ReadType;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A remote read micro benchmark that reads a file in a thread from a remote task. This benchmark
 * task will read the file written by the {@link SimpleWriteDefinition}, so each thread will read
 * the file simple-read-write/[target-task-id]/[thread-id].
 *
 * [target-task-id] = (readTargetTaskId != -1) ? readTargetTaskId
 *                                             : ((workerId + readTargetOffset) % totalNumWorkers)
 *
 */
public final class RemoteReadDefinition extends
    AbstractBenchmarkJobDefinition<RemoteReadConfig, Long, IOThroughputResult> {
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mReadBytesQueue = null;

  /**
   * Constructs a new {@link RemoteReadDefinition}.
   */
  public RemoteReadDefinition() {}

  @Override
  public Map<WorkerInfo, Long> selectExecutors(RemoteReadConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, Long> result = new TreeMap<>(new Comparator<WorkerInfo>() {
      @Override
      public int compare(WorkerInfo o1, WorkerInfo o2) {
        if (o1.getId() > o2.getId()) {
          return 1;
        } else if (o1.getId() == o2.getId()) {
          return 0;
        } else {
          return -1;
        }
      }
    });

    for (WorkerInfo workerInfo : workerInfoList) {
      long readTarget = workerInfo.getId();
      if (config.getReadTargetTaskId() != -1) {
        readTarget = config.getReadTargetTaskId();
      } else {
        readTarget = (readTarget + config.getReadTargetTaskOffset()) % workerInfoList.size();
      }
      result.put(workerInfo, readTarget);
    }
    return result;
  }

  @Override
  public String join(RemoteReadConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults);
  }

  @Override
  protected synchronized void before(RemoteReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // instantiates the queue
    if (mReadBytesQueue == null) {
      mReadBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(RemoteReadConfig config, Long targetTaskId,
      JobWorkerContext jobWorkerContext, int batch) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = SimpleWriteDefinition.getWritePrefix(fs, jobWorkerContext) + "/"
        + Thread.currentThread().getId() % config.getThreadNum();

    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    ReadType readType = config.getReadType();

    long readBytes = readFile(fs, path, (int) bufferSize, readType);
    mReadBytesQueue.add(readBytes);
  }

  /**
   * Reads a Alluxio file with given configurations.
   *
   * @param fs the file system
   * @param path the Alluxio file's full path
   * @param bufferSize the read buffer size
   * @param readType the read type
   * @return the read length
   * @throws Exception when the file open or read failed
   */
  private long readFile(AbstractFS fs, String path, int bufferSize, ReadType readType)
      throws Exception {
    long readLen = 0;
    byte[] content = new byte[bufferSize];
    InputStream is = fs.open(path, readType);
    int lastReadSize = is.read(content);
    while (lastReadSize > 0) {
      readLen += lastReadSize;
      lastReadSize = is.read(content);
    }
    is.close();
    return readLen;
  }

  @Override
  protected void after(RemoteReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the directory used by SimpleWrite.
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = SimpleWriteDefinition.getWritePrefix(fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected IOThroughputResult process(RemoteReadConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions
        .checkArgument(benchmarkThreadTimeList.size() == 1, "RemoteRead only does one batch");
    // calc the average time
    long totalTimeNS = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTimeNS += time;
    }
    long totalBytes = 0;
    for (long bytes : mReadBytesQueue) {
      totalBytes += bytes;
    }
    // release the queue
    mReadBytesQueue = null;
    double throughput = (totalBytes / (double) Constants.MB)
        / (totalTimeNS / (double) Constants.SECOND_NANO);
    double averageTimeMS = totalTimeNS / (double) benchmarkThreadTimeList.size()
        / Constants.SECOND_NANO * Constants.SECOND_MS;
    return new IOThroughputResult(throughput, averageTimeMS);
  }
}
