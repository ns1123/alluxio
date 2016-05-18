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
import alluxio.client.WriteType;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.job.fs.HDFSFS;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A simple write micro benchmark that writes a file in a thread. Each thread writes the file to
 * simple-read-write/[task-id]/[thread-id]. Note that the benchmark will clean up the written files
 * only if {@link SimpleWriteConfig#isCleanUp()} is {@code true}.
 */
public final class SimpleWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SimpleWriteConfig, IOThroughputResult> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  public static final String READ_WRITE_DIR = "/simple-read-write/";

  /**
   * Constructs a new {@link SimpleWriteDefinition}.
   */
  public SimpleWriteDefinition() {}

  @Override
  public String join(SimpleWriteConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults);
  }

  @Override
  protected void before(SimpleWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    // delete the directory if it exists
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);
  }

  @Override
  protected void run(SimpleWriteConfig config, Void args, JobWorkerContext jobWorkerContext,
      int batch) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    // use the thread id as the file name
    String path = getWritePrefix(fs, jobWorkerContext) + "/"
        + Thread.currentThread().getId() % config.getThreadNum();

    long blockSize = FormatUtils.parseSpaceSize(config.getBlockSize());
    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    long fileSize = FormatUtils.parseSpaceSize(config.getFileSize());
    WriteType writeType = config.getWriteType();
    OutputStream os;
    if (fs instanceof HDFSFS) {
      os = fs.create(path, config.getHdfsReplication());
    } else {
      os = fs.create(path, blockSize, writeType);
    }

    // write the file
    byte[] content = new byte[(int) bufferSize];
    Arrays.fill(content, (byte) 'a');
    long remain = fileSize;
    while (remain >= bufferSize) {
      os.write(content);
      remain -= bufferSize;
    }
    if (remain > 0) {
      os.write(content, 0, (int) remain);
    }

    os.close();
  }

  @Override
  protected void after(SimpleWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the directory used by this task.
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected IOThroughputResult process(SimpleWriteConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "SimpleWrite only does one batch");
    // calc the average time
    long totalTimeNS = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTimeNS += time;
    }
    long bytes = FormatUtils.parseSpaceSize(config.getFileSize()) * config.getThreadNum();
    double throughput =
        (bytes / (double) Constants.MB) / (totalTimeNS / (double) Constants.SECOND_NANO);
    double averageTimeMS = totalTimeNS / (double) benchmarkThreadTimeList.size()
        / Constants.SECOND_NANO * Constants.SECOND_MS;
    return new IOThroughputResult(throughput, averageTimeMS);
  }

  /**
   * Gets the write tasks working directory prefix.
   *
   * @param fs the file system
   * @param ctx the job worker context
   * @return the tasks working directory prefix
   */
  public static String getWritePrefix(AbstractFS fs, JobWorkerContext ctx) {
    String path = READ_WRITE_DIR + ctx.getTaskId();
    if (!(fs instanceof AlluxioFS)) {
      path = ctx.getConfiguration().get(Constants.UNDERFS_ADDRESS) + path;
    }
    return new StringBuilder().append(path).toString();
  }
}
