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
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.job.fs.HDFSFS;
import alluxio.job.util.SerializableVoid;
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

  /**
   * Constructs a new {@link SimpleWriteDefinition}.
   */
  public SimpleWriteDefinition() {}

  @Override
  public String join(SimpleWriteConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults,
            DatabaseConstants.MICROBENCH_DURATION_THROUGHPUT);
  }

  @Override
  protected void before(SimpleWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, config.getBlockSize());
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, config.getWriteType());

    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(config.getBaseDir(), fs, jobWorkerContext);
    // delete the directory if it exists
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);
  }

  @Override
  protected void run(SimpleWriteConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    // use the thread id as the file name
    String path = getWritePrefix(config.getBaseDir(), fs, jobWorkerContext) + "/" + threadIndex;

    long blockSize = config.getBlockSize();
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
    String path = getWritePrefix(config.getBaseDir(), fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
    ConfigurationTestUtils.resetConfiguration();
  }

  @Override
  protected IOThroughputResult process(SimpleWriteConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "SimpleWrite only does one batch");
    // calc the average time
    long totalTimeNs = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTimeNs += time;
    }
    long bytes = FormatUtils.parseSpaceSize(config.getFileSize()) * config.getThreadNum();
    double averageThroughputBpns = bytes / (double) totalTimeNs;
    double averageThroughputMbps = (averageThroughputBpns / Constants.MB) * Constants.SECOND_NANO;
    double averageTimeNs = totalTimeNs / (double) config.getThreadNum();
    double averageTimeMs = (averageTimeNs / Constants.SECOND_NANO) * Constants.SECOND_MS;

    return new IOThroughputResult(averageThroughputMbps, averageTimeMs);
  }

  /**
   * Gets the write tasks working directory prefix.
   *
   * @param baseDir the base directory for the test files
   * @param fs the file system
   * @param ctx the job worker context
   * @return the tasks working directory prefix
   */
  public static String getWritePrefix(String baseDir, AbstractFS fs, JobWorkerContext ctx) {
    String path = baseDir + ctx.getTaskId();
    // If the FS is not Alluxio, apply the Alluxio UNDERFS_ADDRESS prefix to the file path.
    // Thereforce, the UFS files are also written to the Alluxio mapped directory.
    if (!(fs instanceof AlluxioFS)) {
      path = Configuration.get(PropertyKey.UNDERFS_ADDRESS) + path;
    }
    return new StringBuilder().append(path).toString();
  }

  @Override
  public Class<SimpleWriteConfig> getJobConfigClass() {
    return SimpleWriteConfig.class;
  }
}
