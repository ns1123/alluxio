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
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.util.SerializableVoid;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A simple read micro benchmark that reads a file in a thread. This benchmark task will read the
 * file written by the {@link SimpleWriteDefinition}, so each thread will read the file
 * simple-read-write/[task-id]/[thread-id].
 */
public final class SimpleReadDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SimpleReadConfig, IOThroughputResult> {
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mReadBytesQueue = null;

  /**
   * Constructs a new {@link SimpleReadDefinition}.
   */
  public SimpleReadDefinition() {}

  @Override
  public String join(SimpleReadConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults,
            DatabaseConstants.MICROBENCH_DURATION_THROUGHPUT);
  }

  @Override
  protected synchronized void before(SimpleReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, config.getReadType());
    // instantiates the queue
    if (mReadBytesQueue == null) {
      mReadBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(SimpleReadConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = SimpleWriteDefinition.getWritePrefix(config.getBaseDir(), fs, jobWorkerContext)
        + "/" + threadIndex;

    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    ReadType readType = config.getReadType();

    long readBytes = readFile(fs, path, (int) bufferSize, readType);
    mReadBytesQueue.add(readBytes);
  }

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
  protected void after(SimpleReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the directory used by SimpleWrite.
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = SimpleWriteDefinition.getWritePrefix(config.getBaseDir(), fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected IOThroughputResult process(SimpleReadConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "SimpleWrite only does one batch");
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

  @Override
  public Class<SimpleReadConfig> getJobConfigClass() {
    return SimpleReadConfig.class;
  }
}
