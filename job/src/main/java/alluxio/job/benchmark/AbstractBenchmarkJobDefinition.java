/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The abstract class for all the benchmark job implementations. This class will launch multiple
 * threads for running the benchmark. It also times the execution per thread.
 *
 * @param <T> the benchmark job configuration
 * @param <P> the benchmark task result type
 */
public abstract class AbstractBenchmarkJobDefinition
    <T extends AbstractBenchmarkJobConfig, P extends BenchmarkTaskResult>
    implements JobDefinition<T, Void, P> {
  @Override
  public Map<WorkerInfo, Void> selectExecutors(T config, List<WorkerInfo> workerInfoList,
      JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, Void> result = new HashMap<>();
    for (WorkerInfo workerInfo : workerInfoList) {
      result.put(workerInfo, (Void) null);
    }
    return result;
  }

  @Override
  public P runTask(T config, Void args, JobWorkerContext jobWorkerContext) throws Exception {
    before(config, jobWorkerContext);
    ExecutorService service = Executors.newFixedThreadPool(config.getThreadNum());
    List<List<Long>> result = new ArrayList<>();
    for (int i = 0; i < config.getBatchNum(); i++) {
      List<Callable<Long>> todo = new ArrayList<>(config.getThreadNum());
      for (int j = 0; j < config.getThreadNum(); j++) {
        todo.add(new BenchmarkThread(config, jobWorkerContext, i));
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

  protected class BenchmarkThread implements Callable<Long> {

    private T mConfig;
    private JobWorkerContext mJobWorkerContext;
    private int mBatch;

    BenchmarkThread(T config, JobWorkerContext jobWorkerContext, int batch) {
      mConfig = config;
      mJobWorkerContext = jobWorkerContext;
      mBatch = batch;
    }

    @Override
    public Long call() throws Exception {
      long startTimeNano = System.nanoTime();
      run(mConfig, mJobWorkerContext, mBatch);
      long endTimeNano = System.nanoTime();
      after(mConfig, mJobWorkerContext);
      return endTimeNano - startTimeNano;
    }
  }

  /**
   * All the preparation work before running the benchmark goes here.
   */
  protected abstract void before(T config, JobWorkerContext jobWorkerContext) throws Exception;

  /**
   * The benchmark implementation goes here. this function is timed by the benchmark thread.
   */
  protected abstract void run(T config, JobWorkerContext jobWorkerContext, int batch)
      throws Exception;

  /**
   * The cleanup work after the benchmark goes here.
   */
  protected abstract void after(T config, JobWorkerContext jobWorkerContext) throws Exception;

  /**
   * Calculates the benchmark result from the timings of the task's threads.
   *
   * @param config the configuration
   * @param benchmarkThreadTimeList the list of time in millisecond of benchmark execution per
   *        thread
   * @return the calculated result
   */
  protected abstract P process(T config, List<List<Long>> benchmarkThreadTimeList);
}
