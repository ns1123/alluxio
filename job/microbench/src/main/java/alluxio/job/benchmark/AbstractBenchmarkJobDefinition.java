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
import alluxio.job.JobWorkerContext;
import alluxio.job.JobDefinition;
import alluxio.util.ShellUtils;

import com.google.common.base.Throwables;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The abstract class for all the benchmark job implementations. This class will launch multiple
 * threads for running the benchmark. It also times the execution per thread.
 *
 * @param <T> the benchmark job configuration
 * @param <P> the benchmark task arg
 * @param <R> the benchmark task result type
 */
public abstract class AbstractBenchmarkJobDefinition<T extends AbstractBenchmarkJobConfig,
    P extends Serializable, R extends BenchmarkTaskResult> implements JobDefinition<T, P, R> {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public R runTask(T config, P args, JobWorkerContext jobWorkerContext) throws Exception {
    before(config, jobWorkerContext);
    cleanUpOsCache();
    ExecutorService service = Executors.newFixedThreadPool(config.getThreadNum());
    List<List<Long>> result = new ArrayList<>();
    for (int i = 0; i < config.getBatchNum(); i++) {
      List<Callable<Long>> todo = new ArrayList<>(config.getThreadNum());
      for (int j = 0; j < config.getThreadNum(); j++) {
        todo.add(new BenchmarkThread(config, args, jobWorkerContext, i, j));
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
    // Run user defined clean up.
    if (config.isCleanUp()) {
      after(config, jobWorkerContext);
    }
    return process(config, result);
  }

  protected class BenchmarkThread implements Callable<Long> {

    private T mConfig;
    private P mArgs;
    private JobWorkerContext mJobWorkerContext;
    private int mBatch;
    private int mThreadIndex;

    /**
     * Creates the benchmark thread.
     *
     * @param config the benchmark configuration
     * @param args the args passed to the benchmark
     * @param jobWorkerContext the job worker context
     * @param batch the no. of the batch
     * @param threadIndex the index of the thread in the current batch
     */
    BenchmarkThread(T config, P args, JobWorkerContext jobWorkerContext, int batch, int threadIndex) {
      mConfig = config;
      mArgs = args;
      mJobWorkerContext = jobWorkerContext;
      mThreadIndex = threadIndex;
      mBatch = batch;
    }

    @Override
    public Long call() throws Exception {
      long startTimeNano = System.nanoTime();
      run(mConfig, mArgs, mJobWorkerContext, mBatch, mThreadIndex);
      // Ensure that data is flushed to disk to ensure that the benchmark doesn't cheat on us with
      // the buffer cache.
      sync();
      long endTimeNano = System.nanoTime();
      return endTimeNano - startTimeNano;
    }
  }

  /**
   * All the preparation work before running the benchmark goes here.
   */
  protected abstract void before(T config, JobWorkerContext jobWorkerContext) throws Exception;

  /**
   * The benchmark implementation goes here. this function is timed by the benchmark thread. A
   * thread index is passed for identifying the index thread running in the batch.
   */
  protected abstract void run(T config, P args, JobWorkerContext jobWorkerContext, int batch, int threadIndex)
      throws Exception;

  /**
   * The cleanup work after the benchmark goes here.
   */
  protected abstract void after(T config, JobWorkerContext jobWorkerContext) throws Exception;

  /**
   * Calculates the benchmark result from the timings of the task's threads.
   *
   * @param config the configuration
   * @param benchmarkThreadTimeList the list of time in nanosecond of benchmark execution per
   *        thread
   * @return the calculated result
   */
  protected abstract R process(T config, List<List<Long>> benchmarkThreadTimeList);

  /**
   * Clean up OS cache.
   */
  private void cleanUpOsCache() {
    if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_WINDOWS) {
      LOG.debug("Not running on linux so not clearing buffer cache");
      return;
    }
    try {
      sync();
      LOG.info("memory before dropping buffer cache:\n{}", free());
      ShellUtils.execCommand("/bin/sh", "-c",
          "echo \"echo 3 > /proc/sys/vm/drop_caches\" | sudo /bin/sh");
      LOG.info("memory after dropping buffer cache:\n{}", free());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String free() {
    try {
      return ShellUtils.execCommand("/usr/bin/free");
    } catch (IOException e) {
      LOG.warn("Failed to call free: {}", e);
      return "unknown";
    }
  }

  private void sync() {
    try {
      ShellUtils.execCommand("/bin/sync");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
