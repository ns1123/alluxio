/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.maxfile;

import alluxio.client.WriteType;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.DatabaseConstants;
import alluxio.job.benchmark.AbstractNoArgBenchmarkJobDefinition;
import alluxio.job.benchmark.BenchmarkEntry;
import alluxio.job.fs.AlluxioFS;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This benchmark measures the max number of files that Alluxio master can hold. It keeps writing
 * empty files to Alluxio until the master no longer responses.
 */
public class MaxFileDefinition
    extends AbstractNoArgBenchmarkJobDefinition<MaxFileConfig, MaxFileResult> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  public static final String WRITE_DIR = "/max-file/";
  /** A queue tracks the total number of files written per thread. */
  private ConcurrentLinkedQueue<Long> mNumFilesQueue = null;

  /**
   * Constructs a new {@link MaxFileDefinition}.
   */
  public MaxFileDefinition() {}

  @Override
  public Class<MaxFileConfig> getJobConfigClass() {
    return MaxFileConfig.class;
  }

  @Override
  public String join(MaxFileConfig config, Map<WorkerInfo, MaxFileResult> taskResults)
      throws Exception {
    long total = 0;
    for (MaxFileResult result : taskResults.values()) {
      total += result.getNumFiles();
    }
    return new BenchmarkEntry(DatabaseConstants.MAX_FILE, ImmutableList.of("Total"),
        ImmutableList.of("int"), ImmutableMap.<String, Object>of("Total", total)).toJson();
  }

  @Override
  protected void before(MaxFileConfig config, JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioFS fs = AlluxioFS.get();
    String path = WRITE_DIR + jobWorkerContext.getTaskId();
    // delete the directory if it exists
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);

    if (mNumFilesQueue == null) {
      mNumFilesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(MaxFileConfig config, SerializableVoid args, JobWorkerContext jobWorkerContext,
      int batch, int threadIndex) throws Exception {
    AlluxioFS fs = AlluxioFS.get();
    long counter = 0;
    while (true) {
      String path = WRITE_DIR + jobWorkerContext.getTaskId() + "/" + threadIndex + "/" + counter;
      try {
        fs.createEmptyFile(path, WriteType.MUST_CACHE);
      } catch (IOException e) {
        // assuming the only failure is timeout
        mNumFilesQueue.add(counter);
        return;
      }

      if (counter % 10000 == 0) {
        LOG.info("Thread " + threadIndex + " has written " + counter + " files ");
      }

      counter++;
    }
  }

  @Override
  protected void after(MaxFileConfig config, JobWorkerContext jobWorkerContext) throws Exception {
    // Do nothing
  }

  @Override
  protected MaxFileResult process(MaxFileConfig config, List<List<Long>> benchmarkThreadTimeList) {
    long total = 0L;
    for (long num : mNumFilesQueue) {
      total += num;
    }
    return new MaxFileResult(total);
  }

}
