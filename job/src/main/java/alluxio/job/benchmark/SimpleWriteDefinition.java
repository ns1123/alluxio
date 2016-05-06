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
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.job.JobWorkerContext;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple write micro benchmark that writes a file in a thread. Each thread writes the file to
 * simple-read-write/[task-id]/[thread-id]. Note the benchmark does not clean up the written file.
 */
public class SimpleWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SimpleWriteConfig, IOThroughputResult> {
  public static final String READ_WRITE_DIR = "/simple-read-write/";

  @Override
  public String join(SimpleWriteConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
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
  protected void before(SimpleWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // create a directory for the current task
    jobWorkerContext.getFileSystem().createDirectory(
        new AlluxioURI(READ_WRITE_DIR + jobWorkerContext.getTaskId()),
        CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
  }

  @Override
  protected void run(SimpleWriteConfig config, Void args, JobWorkerContext jobWorkerContext,
      int batch) throws Exception {
    FileSystem fileSystem = jobWorkerContext.getFileSystem();
    // use the thread id as the file name
    AlluxioURI uri = new AlluxioURI(READ_WRITE_DIR + jobWorkerContext.getTaskId() + "/"
        + Thread.currentThread().getId() % config.getThreadNum());

    long blockSize = FormatUtils.parseSpaceSize(config.getBlockSize());
    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    long fileSize = FormatUtils.parseSpaceSize(config.getFileSize());
    WriteType writeType = config.getWriteType();
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(blockSize).setWriteType(writeType);
    FileOutStream os = fileSystem.createFile(uri, options);

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
    if (config.getCleanUp()) {
      // Delete the directory used by this task.
      jobWorkerContext.getFileSystem()
          .delete(new AlluxioURI(READ_WRITE_DIR + jobWorkerContext.getTaskId()),
              DeleteOptions.defaults().setRecursive(true));
    }
  }

  @Override
  protected IOThroughputResult process(SimpleWriteConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "SimpleWrite only does one batch");
    // calc the average time
    long totalTime = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTime += time;
    }
    long bytes = FormatUtils.parseSpaceSize(config.getFileSize()) * config.getThreadNum();
    double throughput = (bytes / (double) Constants.MB / Constants.MB) / (totalTime
        / (double) Constants.SECOND_NANO);
    return new IOThroughputResult(throughput);
  }
}
