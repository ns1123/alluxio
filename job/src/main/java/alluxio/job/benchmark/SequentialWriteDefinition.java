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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Write files to Alluxio sequentially to test the writing performance as the number of files or
 * blocks accumulate in Alluxio.
 */
public class SequentialWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SequentialWriteConfig, RuntimeResult> {

  private static final String WRITE_DIR = "/sequential-write/";

  @Override
  public String join(SequentialWriteConfig config, Map<WorkerInfo, RuntimeResult> taskResults)
      throws Exception {
    StringBuilder sb = new StringBuilder();
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
    return sb.toString();
  }

  @Override
  protected void before(SequentialWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    jobWorkerContext.getFileSystem()
        .createDirectory(new AlluxioURI(getWriteDir(jobWorkerContext.getTaskId())),
            CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
  }

  @Override
  protected void run(SequentialWriteConfig config, Void args, JobWorkerContext jobWorkerContext,
      int batch) throws IOException {
    FileSystem fileSystem = jobWorkerContext.getFileSystem();
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(config.getBlockSize())
            .setWriteType(config.getWriteType());

    // Use a fixed buffer size (4KB).
    final long defaultBufferSize = 4096L;
    byte[] content = new byte[(int) Math.min(config.getFileSize(), defaultBufferSize)];
    Arrays.fill(content, (byte) 'a');
    for (int i = 0; i < config.getBatchSize(); i++) {
      AlluxioURI uri = new AlluxioURI(
          Paths.get(getWriteDir(jobWorkerContext.getTaskId()), batch + "-" + i).toString());
      try {
        if (fileSystem.exists(uri)) {
          fileSystem.delete(uri);
        }
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage(), e);
      }
      try (FileOutStream os = fileSystem.createFile(uri, options)) {
        long remaining = config.getFileSize();
        while (remaining >= defaultBufferSize) {
          os.write(content);
          remaining -= defaultBufferSize;
        }
        if (remaining > 0) {
          os.write(content, 0, (int) remaining);
        }
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }

  @Override
  protected void after(SequentialWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    jobWorkerContext.getFileSystem()
        .delete(new AlluxioURI(getWriteDir(jobWorkerContext.getTaskId())),
            DeleteOptions.defaults().setRecursive(true));
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
   * @param taskId the task Id
   * @return the working direcotry for this task
   */
  private static String getWriteDir(int taskId) {
    return Paths.get(WRITE_DIR + taskId).toString();
  }

  private SequentialWriteDefinition() {} // prevent instantiation
}

