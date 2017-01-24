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
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Write files to Alluxio sequentially to test the writing performance as the number of files or
 * blocks accumulate in Alluxio.
 */
public final class SequentialWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SequentialWriteConfig, RuntimeResult> {

  private static final String WRITE_DIR = "/sequential-write/";

  /**
   * Constructs a new {@link SequentialWriteDefinition}.
   */
  public SequentialWriteDefinition() {}

  @Override
  public String join(SequentialWriteConfig config, Map<WorkerInfo, RuntimeResult> taskResults)
      throws Exception {
    String columnName = "Series";
    // assumes there is only one worker; get result from first value
    List<Double> result = taskResults.values().iterator().next().getRuntime();
    return new BenchmarkEntry(DatabaseConstants.SEQUENTIAL_WRITE,
        ImmutableList.of(columnName), ImmutableList.of("text"),
        ImmutableMap.<String, Object>of(columnName, Joiner.on("\n").join(result))).toJson();
  }

  @Override
  protected void before(SequentialWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, config.getBlockSize());
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, config.getWriteType());
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);
  }

  @Override
  protected void run(SequentialWriteConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws IOException {
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
    ConfigurationTestUtils.resetConfiguration();
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
      path = Configuration.get(PropertyKey.UNDERFS_ADDRESS) + path + "/";
    }
    return new StringBuilder().append(path).append("/").toString();
  }

  @Override
  public Class<SequentialWriteConfig> getJobConfigClass() {
    return SequentialWriteConfig.class;
  }
}
