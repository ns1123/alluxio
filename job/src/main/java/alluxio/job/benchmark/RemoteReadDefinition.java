/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

/**
 * A remote read micro benchmark that reads a file in a thread from a remote task. This benchmark
 * task will read the file written by the {@link SimpleWriteDefinition}, so each thread will read
 * the file simple-read-write/[target-task-id]/[thread-id].
 *
 * [target-task-id] = (readTargetTaskId != -1) ? readTargetTaskId
 *                                             : ((workerId + readTargetOffset) % totalNumWorkers)
 *
 */
public final class RemoteReadDefinition implements
    JobDefinition<RemoteReadConfig, Long ,IOThroughputResult> {
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mReadBytesQueue = null;

  @Override
  public Map<WorkerInfo, Long> selectExecutors(RemoteReadConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, Long> result = new HashMap<>();
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
  public IOThroughputResult runTask(RemoteReadConfig config, Long args,
                                    JobWorkerContext jobWorkerContext)
      throws Exception {
    before(config, jobWorkerContext);
    ExecutorService service = Executors.newFixedThreadPool(config.getThreadNum());
    List<List<Long>> result = new ArrayList<>();
    for (int i = 0; i < config.getBatchNum(); i++) {
      List<Callable<Long>> todo = new ArrayList<>(config.getThreadNum());
      for (int j = 0; j < config.getThreadNum(); j++) {
        todo.add(new RemoteReadBenchmarkThread(config, args, jobWorkerContext, i));
      }
      // invoke all and wait for them to finish
      List<Future<Long>> futureResult = service.invokeAll(todo);
      List<Long> executionTimes = new ArrayList<>();
      for (Future<Long> future : futureResult) {
        // if the thread fails, future.get() will throw the execution exception
        executionTimes.add(future.get());
      }
      result.add(executionTimes);
    }
    after(config, jobWorkerContext);
    return process(config, result);
  }

  protected class RemoteReadBenchmarkThread implements Callable<Long> {
    private RemoteReadConfig mConfig;
    private long mArgs;
    private JobWorkerContext mJobWorkerContext;
    private int mBatch;

    RemoteReadBenchmarkThread(RemoteReadConfig config, long args, JobWorkerContext
        jobWorkerContext, int batch) {
      mConfig = config;
      mArgs = args;
      mJobWorkerContext = jobWorkerContext;
      mBatch = batch;
    }

    @Override
    public Long call() throws Exception {
      long startTimeNano = System.nanoTime();
      run(mConfig, mArgs, mJobWorkerContext, mBatch);
      long endTimeNano = System.nanoTime();
      return endTimeNano - startTimeNano;
    }
  }

  /**
   * All the preparation work before running the benchmark goes here.
   */
  private synchronized void before(RemoteReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // instantiates the queue
    if (mReadBytesQueue == null) {
      mReadBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  /**
   * The benchmark implementation goes here. this function is timed by the benchmark thread.
   */
  private void run(RemoteReadConfig config, long targetTaskId,
      JobWorkerContext jobWorkerContext, int batch) throws Exception {
    AlluxioURI uri =
        new AlluxioURI(SimpleWriteDefinition.READ_WRITE_DIR + targetTaskId + "/"
            + Thread.currentThread().getId() % config.getThreadNum());
    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    ReadType readType = config.getReadType();

    long readBytes = readFile(jobWorkerContext.getFileSystem(), uri, (int) bufferSize, readType);
    mReadBytesQueue.add(readBytes);
  }

  /**
   * Reads a Alluxio file with given configurations.
   *
   * @param fs the file system
   * @param uri the Alluxio URI
   * @param bufferSize the read buffer size
   * @param readType the read type
   * @return the read length
   * @throws Exception when the file open or read failed
   */
  private long readFile(FileSystem fs, AlluxioURI uri, int bufferSize, ReadType readType)
      throws Exception {
    long readLen = 0;
    byte[] content = new byte[bufferSize];
    FileInStream is = fs.openFile(uri, OpenFileOptions.defaults().setReadType(readType));
    int lastReadSize = is.read(content);
    while (lastReadSize > 0) {
      readLen += lastReadSize;
      lastReadSize = is.read(content);
    }
    is.close();
    return readLen;
  }

  /**
   * The cleanup work after the benchmark goes here.
   */
  private void after(RemoteReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // do nothing
  }

  /**
   * Calculates the benchmark result from the timings of the task's threads.
   *
   * @param config the configuration
   * @param benchmarkThreadTimeList the list of time in millisecond of benchmark execution per
   *        thread
   * @return the calculated result
   */
  private IOThroughputResult process(RemoteReadConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions
        .checkArgument(benchmarkThreadTimeList.size() == 1, "RemoteRead only does one batch");
    // calc the average time
    long totalTime = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTime += time;
    }
    long totalBytes = 0;
    for (long bytes : mReadBytesQueue) {
      totalBytes += bytes;
    }
    // release the queue
    mReadBytesQueue = null;
    double throughput = (totalBytes / (double) Constants.MB / Constants.MB)
        / (totalTime / (double) Constants.SECOND_NANO);
    return new IOThroughputResult(throughput);
  }

}
