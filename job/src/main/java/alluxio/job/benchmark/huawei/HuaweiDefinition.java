/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.huawei;

import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.AbstractNoArgBenchmarkJobDefinition;
import alluxio.job.benchmark.RuntimeResult;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Write files to Alluxio sequentially to test the writing performance as the number of files or
 * blocks accumulate in Alluxio.
 */
public final class HuaweiDefinition
    extends AbstractNoArgBenchmarkJobDefinition<HuaweiConfig, RuntimeResult> {

  private static final String WRITE_DIR = "/sequential-write/";

  /**
   * Constructs a new {@link HuaweiDefinition}.
   */
  public HuaweiDefinition() {
  }

  @Override
  public String join(HuaweiConfig config, Map<WorkerInfo, RuntimeResult> taskResults)
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
    return sb.toString();
  }

  @Override
  protected void before(HuaweiConfig config, JobWorkerContext jobWorkerContext) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);
  }

  @Override
  protected void run(HuaweiConfig config, Void args, JobWorkerContext jobWorkerContext, int batch)
      throws IOException {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    HuaweiAlluxioFSTest huaweiAlluxioFSTest =
        new HuaweiAlluxioFSTest(getWritePrefix(fs, jobWorkerContext), config.getDepth(),
            config.getWidth(), config.getCount(), config.getSize());

    try {
      switch (config.getOperation()) {
        case READ:
          huaweiAlluxioFSTest.testReadFile(ReadType.CACHE);
          break;
        case SYNC_WRITE:
          huaweiAlluxioFSTest.testWriteFile(WriteType.CACHE_THROUGH);
          break;
        case ASYNC_WRITE:
          huaweiAlluxioFSTest.testWriteFile(WriteType.ASYNC_THROUGH);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported operation.");
      }
    } catch (Exception e) {
      LOG.error("Failed to run Huawei test.", e);
    }
  }

  @Override
  protected void after(HuaweiConfig config, JobWorkerContext jobWorkerContext) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected RuntimeResult process(HuaweiConfig config, List<List<Long>> benchmarkRuntime) {
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
      path = ctx.getConfiguration().get(Constants.UNDERFS_ADDRESS) + path + "/";
    }
    return new StringBuilder().append(path).append("/").toString();
  }
}

