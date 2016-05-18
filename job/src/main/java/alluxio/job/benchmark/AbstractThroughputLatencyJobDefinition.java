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
import alluxio.Constants;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark template that measures the throughput and latency of an operation.
 * @param <T> the configuration type
 */
public abstract class AbstractThroughputLatencyJobDefinition<T extends
    AbstractThroughputLatencyJobConfig>
    implements JobDefinition<T, Void, ThroughputLatency> {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected RateLimiter mRateLimiter = null;

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
  public String join(T config, Map<WorkerInfo, ThroughputLatency> taskResults) throws IOException {
    ThroughputLatency merged = null;
    for (Map.Entry<WorkerInfo, ThroughputLatency> entry : taskResults.entrySet()) {
      if (merged == null) {
        merged = entry.getValue();
      } else {
        merged.add(entry.getValue());
      }
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    printStream.println(config.getName() + " " + config.getUniqueTestId());
    printStream.println("Task Configuration.");
    printStream.println(config.toString());
    printStream.println("Benchmark Result.");

    merged.output(printStream);

    printStream.close();
    outputStream.close();

    String output = outputStream.toString();
    // Output to stdout to avoid spamming the master log since this is being outputted many times.
    System.out.println(output);
    return output;
  }

  @Override
  public ThroughputLatency runTask(T config, Void args, JobWorkerContext jobWorkerContext)
      throws Exception {
    before(config, jobWorkerContext);
    ExecutorService service = Executors.newFixedThreadPool(config.getThreadNum());

    ThroughputLatency throughputLatency = new ThroughputLatency();
    // Run the tasks in the configured rate.
    for (int i = 0; i < config.getLoad(); i++) {
      mRateLimiter.acquire();
      service.submit(new BenchmarkClosure(config, jobWorkerContext, throughputLatency, i));
    }
    // Wait for a long till it succeeds.
    try {
      service.shutdown();
      // TODO(peis): Implement job cancellation or make this configurable and available in all
      // benchmarks.
      service.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw e;
    }
    if (config.isCleanUp()) {
      after(config, jobWorkerContext);
    }
    return throughputLatency;
  }

  /** The closure to run the benchmark. */
  protected class BenchmarkClosure implements Runnable {
    private T mConfig;
    private JobWorkerContext mJobWorkerContext;
    private ThroughputLatency mThroughputLatency;
    private int mCommandId;

    public BenchmarkClosure(T config, JobWorkerContext jobWorkerContext,
        ThroughputLatency throughputLatency, int commandId) {
      mConfig = config;
      mJobWorkerContext = jobWorkerContext;
      mThroughputLatency = throughputLatency;
      mCommandId = commandId;
    }

    @Override
    public void run() {
      long startTimeNano = System.nanoTime();
      boolean success = execute(mConfig, mJobWorkerContext, mCommandId);
      long endTimeNano = System.nanoTime();

      mThroughputLatency.record(startTimeNano, endTimeNano, success);
    }
  }

  /**
   * Runs before a test.
   *
   * @param config the config
   * @param jobWorkerContext worker context
   * @throws Exception
   */
  protected void before(T config, JobWorkerContext jobWorkerContext) throws Exception {
    try {
      jobWorkerContext.getFileSystem()
          .createDirectory(new AlluxioURI(getWorkDir(config, jobWorkerContext.getTaskId())),
              CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
    } catch (Exception e) {
      LOG.info("Failed to create working directory: " + e.getMessage());
      throw e;
    }
    mRateLimiter = RateLimiter.create(config.getExpectedThroughput());
  }

  /**
   * The benchmark implementation goes here. The throughput and latency of this function is
   * recorded.
   *
   * @param config the config
   * @param jobWorkerContext the worker context
   * @param commandId the unique Id of this execution
   * @return true if the execution succeeded
   */
  protected abstract boolean execute(T config, JobWorkerContext jobWorkerContext, int commandId);

  /**
   * Runs after the test to clean up the state.
   *
   * @param config the config
   * @param jobWorkerContext the worker context
   * @throws Exception
   */
  protected void after(T config, JobWorkerContext jobWorkerContext) throws Exception {
    try {
      jobWorkerContext.getFileSystem()
          .delete(new AlluxioURI(getWorkDir(config, jobWorkerContext.getTaskId())),
              DeleteOptions.defaults().setRecursive(true));
    } catch (Exception e) {
      LOG.info("Failed to cleanup.", e);
      throw e;
    }
  }

  /**
   * @param taskId the task Id
   * @return the working direcotry for this task
   */
  protected String getWorkDir(T config, int taskId) {
    StringBuilder sb = new StringBuilder();
    sb.append("/");
    sb.append(config.getName());
    sb.append("/");
    sb.append(taskId);
    return sb.toString();
  }
}
