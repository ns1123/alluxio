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
import alluxio.client.WriteType;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write files to Alluxio sequentially to test the writing performance as the number of files or
 * blocks accumulate in Alluxio.
 */
public final class SequentialWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SequentialWriteConfig, RuntimeResult> {

  private static final String WRITE_DIR = "/sequential-write/";

  // This is used to make sure that the loggin in join only run once.
  // TODO(peis): Remove this hack by avoiding running join more than once.
  private AtomicInteger mLogCount = new AtomicInteger(0);

  /**
   * Constructs a new {@link SequentialWriteDefinition}.
   */
  public SequentialWriteDefinition() {}

  @Override
  public String join(SequentialWriteConfig config, Map<WorkerInfo, RuntimeResult> taskResults)
      throws Exception {
    StringBuilder sb = new StringBuilder();

    // Add dummy result so that autobot doesn't crash.
    // TODO(peis): Get rid of this.
    sb.append("Throughput:1 (MB/s)\n");
    sb.append("Duration:1 (ms)\n");

    sb.append(config.getName() + " " + config.getUniqueTestId());
    sb.append("********** Task Configurations **********\n");
    sb.append(config.toString());
    sb.append("********** Statistics **********\n");

    for (Entry<WorkerInfo, RuntimeResult> entry : taskResults.entrySet()) {
      sb.append(
          "Runtime(seconds)@" + entry.getKey().getId() + "@" + entry.getKey().getAddress().getHost()
              + "\n");
      List<Double> runtime = entry.getValue().getRuntime();
      for (Double t : runtime) {
        sb.append(t + "\n");
      }
    }
    if (mLogCount.compareAndSet(0, 1)) {
      System.out.println(sb.toString());
    }
    return sb.toString();
  }

  @Override
  protected void before(SequentialWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);
  }

  @Override
  protected void run(SequentialWriteConfig config, Void args, JobWorkerContext jobWorkerContext,
      int batch) throws IOException {
    AbstractFS fs = config.getFileSystemType().getFileSystem();

    long blockSize = config.getBlockSize();
    WriteType writeType = config.getWriteType();
    // Use a fixed buffer size (4KB).
    final long defaultBufferSize = 4096L;
    byte[] content = new byte[(int) Math.min(config.getFileSize(), defaultBufferSize)];
    Arrays.fill(content, (byte) 'a');
    for (int i = 0; i < config.getBatchSize(); i++) {
      String path = getWritePrefix(fs, jobWorkerContext) + batch + "-" + i;
      OutputStream os = fs.create(path, blockSize, writeType);
      long remaining = config.getFileSize();
      while (remaining >= defaultBufferSize) {
        os.write(content);
        remaining -= defaultBufferSize;
      }
      if (remaining > 0) {
        os.write(content, 0, (int) remaining);
      }
      os.close();
    }
  }

  @Override
  protected void after(SequentialWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected RuntimeResult process(SequentialWriteConfig config, List<List<Long>> benchmarkRuntime) {
    List<Double> runtime = new ArrayList<>();
    for (List<Long> r : benchmarkRuntime) {
      Preconditions.checkState(r.size() == 1, "SequentialWrite only uses 1 thread.");
      runtime.add(r.get(0) / (double) Constants.SECOND_NANO);
    }

    return new RuntimeResult(runtime);
  }

  /**
   * Gets the write tasks working directory prefix.
   *
   * @param fs the file system
   * @param ctx the job worker context
   * @return the tasks working directory prefix
   */
  private String getWritePrefix(AbstractFS fs, JobWorkerContext ctx) {
    String path = WRITE_DIR + ctx.getTaskId();
    if (!(fs instanceof AlluxioFS)) {
      path = Configuration.get(Constants.UNDERFS_ADDRESS) + path + "/";
    }
    return new StringBuilder().append(path).append("/").toString();
  }
}
