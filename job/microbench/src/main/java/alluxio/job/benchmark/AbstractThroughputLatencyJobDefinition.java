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
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.JobDefinition;
import alluxio.job.fs.JobUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark template that measures the throughput and latency of an operation.
 * @param <T> the configuration type
 */
public abstract class AbstractThroughputLatencyJobDefinition<T extends AbstractThroughputLatencyJobConfig>
    implements JobDefinition<T, Integer, ThroughputLatency> {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected RateLimiter mRateLimiter = null;

  // The shuffled integers ranging from 0 to load - 1.
  private List<Integer> mShuffled = null;

  @Override
  public Map<WorkerInfo, Integer> selectExecutors(T config, List<WorkerInfo> workerInfoList,
      JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, Integer> result = new TreeMap<>(JobUtils.createWorkerInfoComparator());
    for (WorkerInfo workerInfo : workerInfoList) {
      result.put(workerInfo,  workerInfoList.size());
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
    // The stream to generate the comments.
    ByteArrayOutputStream commentStream = new ByteArrayOutputStream();
    PrintStream commentPrintStream = new PrintStream(commentStream);
    commentPrintStream.println(config.getName() + " " + config.getUniqueTestId());
    commentPrintStream.println("Task Configuration.");
    commentPrintStream.println(config.toString());
    commentPrintStream.println("Benchmark Result.");
    merged.output(commentPrintStream);

    commentPrintStream.close();
    commentStream.close();
    // TODO(peis): Remove this once autobots can support better comment format.
    String comment = commentStream.toString().replace("\n", "LINESEPARATOR");

    return merged.createBenchmarkEntry(config.getName(), comment).toJson();
  }

  @Override
  public ThroughputLatency runTask(T config, Integer numTasks, JobWorkerContext jobWorkerContext)
      throws Exception {
    before(config, jobWorkerContext, numTasks);
    ExecutorService service = Executors.newFixedThreadPool(config.getThreadNum());

    ThroughputLatency throughputLatency = new ThroughputLatency();
    // Run the tasks in the configured rate.
    for (int i = 0; i < config.getLoad(); i++) {
      mRateLimiter.acquire();
      service.submit(new BenchmarkClosure(config, jobWorkerContext, throughputLatency,
          numTasks, config.isShuffleLoad() ? mShuffled.get(i) : i));
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
      after(config, jobWorkerContext, numTasks);
    }
    return throughputLatency;
  }

  /** The closure to run the benchmark. */
  protected class BenchmarkClosure implements Runnable {
    private T mConfig;
    private JobWorkerContext mJobWorkerContext;
    private ThroughputLatency mThroughputLatency;
    private int mNumTasks;
    private int mCommandId;

    public BenchmarkClosure(T config, JobWorkerContext jobWorkerContext,
        ThroughputLatency throughputLatency, int numTasks, int commandId) {
      mConfig = config;
      mJobWorkerContext = jobWorkerContext;
      mThroughputLatency = throughputLatency;
      mNumTasks = numTasks;
      mCommandId = commandId;
    }

    @Override
    public void run() {
      long startTimeNano = System.nanoTime();
      boolean success = execute(mConfig, mJobWorkerContext, mNumTasks, mCommandId);
      long endTimeNano = System.nanoTime();
      synchronized (mThroughputLatency) {
        mThroughputLatency.record(startTimeNano, endTimeNano, success);
      }
    }
  }

  /**
   * Runs before a test.
   *
   * @param config the config
   * @param jobWorkerContext worker context
   * @param numTasks the number of tasks
   * @throws Exception
   */
  protected void before(T config, JobWorkerContext jobWorkerContext, int numTasks)
      throws Exception {
    try {
      config.getFileSystemType().getFileSystem()
          .createDirectory(getWorkDir(config, jobWorkerContext.getTaskId(), numTasks),
                config.getWriteType());
    } catch (Exception e) {
      LOG.info("Failed to create working directory: " + e.getMessage());
      throw e;
    }
    mRateLimiter = RateLimiter.create(config.getExpectedThroughput());
    if (config.isShuffleLoad()) {
      mShuffled = new ArrayList<>(config.getLoad());
      for (int i = 0; i < config.getLoad(); i++) {
        mShuffled.add(i);
      }
      Collections.shuffle(mShuffled);
    }
  }

  /**
   * The benchmark implementation goes here. The throughput and latency of this function is
   * recorded.
   *
   * @param config the config
   * @param jobWorkerContext the worker context
   * @param numTasks the number of tasks
   * @param commandId the unique Id of this execution
   * @return true if the execution succeeded
   */
  protected abstract boolean execute(T config, JobWorkerContext jobWorkerContext, int numTasks,
      int commandId);

  /**
   * Runs after the test to clean up the state.
   *
   * @param config the config
   * @param jobWorkerContext the worker context
   * @param numTasks the number of tasks
   * @throws Exception
   */
  protected void after(T config, JobWorkerContext jobWorkerContext, int numTasks) throws Exception {
    try {
      config.getFileSystemType().getFileSystem()
          .delete(getWorkDir(config, jobWorkerContext.getTaskId(), numTasks), true);
    } catch (Exception e) {
      LOG.info("Failed to cleanup.", e);
      throw e;
    }
  }

  /**
   * @param config the config
   * @param taskId the task Id
   * @param numTasks the number of tasks
   * @return the working directory for this task
   */
  protected String getWorkDir(T config, int taskId, int numTasks) {
    StringBuilder sb = new StringBuilder();
    sb.append("/");
    sb.append(config.getWorkDir());
    sb.append("/");
    sb.append(config.isLocal() ? taskId : (taskId + 1) % numTasks);
    return sb.toString();
  }
}
