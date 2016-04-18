/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.job.JobWorkerContext;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A simple read micro benchmark that reads a file in a thread.
 */
public class SimpleReadDefinition
    extends AbstractBenchmarkJobDefinition<SimpleReadConfig, IOThroughputResult> {
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mReadBytesQueue=null;

  @Override
  public String join(SimpleReadConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("********** Task Configurations **********\n");
    sb.append(config.toString());
    sb.append("********** Statistics **********\n");
    sb.append("Worker\t\tThroughput(MB/s)");
    for (Entry<WorkerInfo, IOThroughputResult> entry : taskResults.entrySet()) {
      sb.append(entry.getKey().getId() + "@" + entry.getKey().getAddress().getHost());
      sb.append("\t\t" + entry.getValue().getThroughput());
    }
    return sb.toString();
  }

  @Override
  protected synchronized void before(SimpleReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // instantiates the queue
    if (mReadBytesQueue == null) {
      mReadBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void runBenchmark(SimpleReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AlluxioURI uri =
        new AlluxioURI(SimpleWriteDefinition.READ_WRITE_DIR + jobWorkerContext.getTaskId() + "/"
            + Thread.currentThread().getId() % config.getThreadNum());
    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    ReadType readType = config.getReadType();

    long readBytes = readFile(jobWorkerContext.getFileSystem(), uri, (int) bufferSize, readType);
    mReadBytesQueue.add(readBytes);
  }

  private long readFile(FileSystem fs, AlluxioURI uri, int bufferSize, ReadType readType)
      throws Exception {
    long readLen = 0;
    byte[] content = new byte[bufferSize];
    FileInStream is = fs.openFile(uri, OpenFileOptions.defaults().setReadType(readType));
    int onceLen = is.read(content);
    while (onceLen > 0) {
      readLen += onceLen;
      onceLen = is.read(content);
    }
    is.close();
    return readLen;
  }

  @Override
  protected void after(SimpleReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // do nothing
  }

  @Override
  protected IOThroughputResult calcResult(SimpleReadConfig config,
      List<Long> benchmarkThreadTimeList) {
    // calc the average time
    long totalTime = 0;
    for (long time : benchmarkThreadTimeList) {
      totalTime += time;
    }
    long totalBytes = 0;
    for (long bytes : mReadBytesQueue) {
      totalBytes += bytes;
    }
    // release the queue
    mReadBytesQueue = null;
    double throughput = (totalBytes / 1024.0 / 1024.0) / (totalTime / 1000.0);
    return new IOThroughputResult(throughput);
  }

}
